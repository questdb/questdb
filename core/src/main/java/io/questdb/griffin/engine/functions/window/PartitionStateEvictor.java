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

package io.questdb.griffin.engine.functions.window;

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import org.jetbrains.annotations.Nullable;

/**
 * Helper for the live view frontier-gated compaction sweep. Rebuilds a
 * partitioned window function's primary {@link Map} into a caller-provided
 * scratch Map by copying only entries whose key survives the sweep; the result
 * is a packed Map with the same key/value layout minus the dropped keys.
 * <p>
 * The API keeps map ownership with the caller: it allocates the scratch, calls
 * {@link #rebuildKeepingMembers}, then swaps references. This lets each window
 * function reuse its scratch across sweeps and size it with a capacity hint
 * derived from its own primary-size knowledge, avoiding intermediate rehashes
 * during the copy.
 * <p>
 * Cost is O(survivingKeys.size()) per invocation — the sweep walks the survivor
 * set and probes the primary for each key, so it never touches the entries it is
 * about to drop. If sweeps routinely drop only a small fraction of keys, adding a
 * per-entry remove primitive to the {@link Map} interface (tombstone-based for
 * hash maps, list-compaction for {@code OrderedMap}) would reduce cost to
 * O(evictedKeys) and remove the scratch map entirely.
 */
public final class PartitionStateEvictor {

    private PartitionStateEvictor() {
    }

    /**
     * Walks {@code survivingKeys} and copies each entry {@code src} still holds for that
     * key into {@code dst}; every {@code src} entry whose key is absent from
     * {@code survivingKeys} is dropped by never being copied. Returns the number of
     * entries copied.
     * <p>
     * {@code dst} must be empty on entry and share {@code src}'s key/value layout, since
     * the caller promotes it to the live map afterwards. {@code survivingKeys} holds the
     * survivor set and is read, never written.
     * <p>
     * No {@link Map} implementation has to match any other. The two key writes go through
     * {@link MapKey#put(io.questdb.cairo.sql.Record, RecordSink)}, which writes via the
     * per-column {@code RecordSinkSPI} putters every implementation supports, rather than
     * {@link MapRecord#copyToKey(MapKey)}, which casts to the concrete implementation's
     * key. That matters because {@code MapFactory.createUnorderedMap} selects on value
     * size as well as key shape, so the anchor map and a window function's partition map
     * legitimately diverge (the anchor map's value is 10 bytes; {@code covar_samp}'s is
     * 49). {@code survivingKeySink} reads the partition-by columns straight off
     * {@code survivingKeys}' own {@link MapRecord} — map records lay value columns out
     * first and key columns after them, so the sink targets the tail slice.
     * <p>
     * The value copy is equally implementation-agnostic: every {@link Map} implementation
     * backs {@link MapValue} with the same {@code FlyweightPackedMapValue}, whose
     * {@link MapValue#copyFrom(MapValue)} is a flat {@code memcpy} of the value block. It
     * therefore requires an identical value layout but no shared implementation, and it
     * writes every value byte, so {@code dst} never depends on {@code createValue()}
     * zero-filling (which {@code OrderedMap} does not guarantee).
     * <p>
     * The copy deliberately uses {@link MapKey#createValue()} rather than the
     * hash-carrying overload: hash functions differ per implementation (for a 4-byte key
     * {@code Hash.hashInt64} zero-extends where {@code Hash.hashMem64} sign-extends), so
     * a hash borrowed from another map would place entries that {@code findValue()} could
     * never locate again. Each map computes its own.
     * <p>
     * The live-view anchor runtime uses this to keep each anchored window
     * function's partition map in lockstep with the anchor map after a
     * frontier-gated sweep drops partitions whose bucket has fallen behind.
     * <p>
     * A function that owns a native ring arena reaches this through the
     * {@link EntryCopier} overload. Reclaiming its MAP entries alone would not be enough and
     * would not even be safe: it holds one ring slab per partition in its own
     * {@code MemoryARW} arena, and a value copy carries the slab's {@code (startOffset,
     * capacity)} pair verbatim, so a rebuilt entry keeps naming the arena the sweep is about
     * to compact. The copier is what re-homes each surviving slab and rewrites the offset that
     * names it; an evicted partition's slab is reclaimed by never being copied at all. See
     * {@code BasePartitionedWindowFunction.compactRingArena()} for the arena side of the same
     * sweep.
     */
    public static long rebuildKeepingMembers(Map src, Map dst, Map survivingKeys, RecordSink survivingKeySink) {
        return rebuildKeepingMembers(src, dst, survivingKeys, survivingKeySink, null);
    }

    /**
     * Overload that hands every surviving entry to {@code copier} after the flat value copy,
     * for a function whose value carries a handle into memory the value copy does not reach.
     * A ring function's slab is the one such handle: {@link MapValue#copyFrom(MapValue)} moves
     * the {@code (startOffset, capacity)} pair verbatim, so without this the rebuilt entry
     * still names the OLD arena's offset, and the sweep's whole point - releasing what the
     * evicted partitions held - would leave those offsets dangling the moment the arena is
     * compacted underneath them.
     * <p>
     * The callback runs against BOTH values while both are live, which is what lets it copy the
     * slab out of {@code src}'s arena and rewrite {@code dst}'s offset to name where it landed.
     * It is called only for entries that survive; an evicted partition's slab is reclaimed by
     * not being copied at all, which is what makes the compacted arena hold exactly the
     * survivors.
     */
    public static long rebuildKeepingMembers(
            Map src,
            Map dst,
            Map survivingKeys,
            RecordSink survivingKeySink,
            @Nullable EntryCopier copier
    ) {
        MapRecordCursor cursor = survivingKeys.getCursor();
        MapRecord survivorRecord = survivingKeys.getRecord();
        long kept = 0;
        while (cursor.hasNext()) {
            MapKey srcKey = src.withKey();
            srcKey.put(survivorRecord, survivingKeySink);
            // A survivor this function never saw a row for has no state to carry over.
            MapValue srcValue = srcKey.findValue();
            if (srcValue == null) {
                continue;
            }
            MapKey dstKey = dst.withKey();
            dstKey.put(survivorRecord, survivingKeySink);
            // srcValue stays valid across the dst insert: the two maps own separate
            // flyweights over separate memory, so a dst resize cannot move it.
            MapValue dstValue = dstKey.createValue();
            dstValue.copyFrom(srcValue);
            if (copier != null) {
                copier.onEntryRetained(srcValue, dstValue);
            }
            kept++;
        }
        return kept;
    }

    /**
     * Callback for the value bytes {@link MapValue#copyFrom(MapValue)} cannot carry on its own.
     * Implemented by the retaining function itself rather than allocated per sweep, so the
     * rebuild stays allocation-free.
     */
    @FunctionalInterface
    public interface EntryCopier {

        /**
         * Called once per surviving entry, after {@code dstValue} has received the flat copy of
         * {@code srcValue} and while both remain valid. Implementations rewrite whatever slots
         * of {@code dstValue} name memory that the flat copy left pointing at the source.
         */
        void onEntryRetained(MapValue srcValue, MapValue dstValue);
    }
}
