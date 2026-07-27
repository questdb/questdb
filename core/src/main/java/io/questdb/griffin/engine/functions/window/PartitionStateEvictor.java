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
 * Cost is O(primary.size()) per invocation regardless of the survivor ratio —
 * every entry is probed for membership. If sweeps routinely drop only a small
 * fraction of keys, adding a per-entry remove primitive to the {@link Map}
 * interface (tombstone-based for hash maps, list-compaction for
 * {@code OrderedMap}) would reduce cost to O(evictedKeys) and remove the
 * scratch map entirely.
 */
public final class PartitionStateEvictor {

    private PartitionStateEvictor() {
    }

    /**
     * Copies every key in {@code survivingKeys} into {@code dst}, which must be empty on
     * entry. {@code survivingKeySink} reads the key columns straight off
     * {@code survivingKeys}' own {@link MapRecord} — map records lay value columns out
     * first and key columns after them, so the sink targets the tail slice.
     * <p>
     * Unlike {@link MapRecord#copyToKey(MapKey)}, {@link MapKey#put(io.questdb.cairo.sql.Record, RecordSink)}
     * writes through the per-column {@code RecordSinkSPI} putters that every {@link Map}
     * implementation supports, so {@code dst} does NOT have to share an implementation
     * with {@code survivingKeys}. That is what lets a caller mirror the anchor map's
     * survivors into a probe built with a different implementation.
     * <p>
     * The copy deliberately uses {@link MapKey#createValue()} rather than the
     * hash-carrying overload: hashes are implementation-specific (see
     * {@link #rebuildKeepingMembers}), so {@code dst} must compute its own.
     * <p>
     * Only keys are read, but {@code createValue()} still materialises {@code dst}'s full
     * value block per entry, so the probe costs the caller's value width per survivor. A
     * value-less probe is not an option: the value layout is what makes {@code dst} select
     * the same {@link Map} implementation as the map it will be probed against.
     */
    public static void copySurvivorKeys(Map survivingKeys, RecordSink survivingKeySink, Map dst) {
        MapRecordCursor cursor = survivingKeys.getCursor();
        MapRecord record = survivingKeys.getRecord();
        while (cursor.hasNext()) {
            MapKey dstKey = dst.withKey();
            dstKey.put(record, survivingKeySink);
            dstKey.createValue();
        }
    }

    /**
     * Iterates {@code src} and copies each entry whose key is present in
     * {@code survivingKeys} into {@code dst}; entries absent from
     * {@code survivingKeys} are dropped. Returns the number of entries copied.
     * <p>
     * {@code dst} must be empty on entry; {@code survivingKeys} holds the survivor set and
     * is read, never written. BOTH must use the same key layout AND the same {@link Map}
     * implementation as {@code src}, because the membership probe and the copy both go
     * through {@link MapRecord#copyToKey(MapKey)}, which casts to the concrete
     * implementation's key. {@code dst} additionally reuses
     * {@code src}'s {@link MapRecord#keyHashCode()} via {@link MapKey#createValue(long)},
     * and hash functions differ per implementation (for a 4-byte key
     * {@code Hash.hashInt64} zero-extends where {@code Hash.hashMem64} sign-extends), so a
     * foreign hash would place entries that {@code findValue()} can never locate again.
     * Do not relax either requirement.
     * <p>
     * A caller whose survivor set lives in a different implementation must mirror it into a
     * matching probe first — see {@link #copySurvivorKeys}.
     * <p>
     * The live-view anchor runtime uses this to keep each anchored window
     * function's partition map in lockstep with the anchor map after a
     * frontier-gated sweep drops partitions whose bucket has fallen behind.
     */
    public static long rebuildKeepingMembers(Map src, Map dst, Map survivingKeys) {
        MapRecordCursor cursor = src.getCursor();
        MapRecord record = src.getRecord();
        long kept = 0;
        while (cursor.hasNext()) {
            MapKey probeKey = survivingKeys.withKey();
            record.copyToKey(probeKey);
            if (probeKey.findValue() == null) {
                continue;
            }
            long srcKeyHash = record.keyHashCode();
            MapKey dstKey = dst.withKey();
            record.copyToKey(dstKey);
            MapValue dstValue = dstKey.createValue(srcKeyHash);
            record.copyValue(dstValue);
            kept++;
        }
        return kept;
    }
}
