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

import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;

/**
 * Helper for live view Phase 5 partition-state eviction. Rebuilds a partitioned
 * window function's primary {@link Map} into a caller-provided scratch Map by
 * copying only entries whose last-activity-ts value column is at or above the
 * retention cutoff. Entries below the cutoff are skipped; the result is a packed
 * Map with the same key/value layout minus the stale keys.
 * <p>
 * The API keeps map ownership with the caller: it allocates the scratch, calls
 * {@link #rebuildKeeping}, then swaps references and frees the old primary. This
 * lets each window function size its scratch with a capacity hint derived from
 * its own primary-size knowledge, avoiding intermediate rehashes during the copy.
 * <p>
 * Cost is O(primary.size()) per invocation regardless of the survivor ratio —
 * every entry is probed to read its last-activity-ts. TODO(live-view): revisit
 * once real-world partition cardinalities are observed. If evictions routinely
 * drop only a small fraction of keys, adding a per-entry remove primitive to
 * the {@link Map} interface (tombstone-based for hash maps, list-compaction for
 * {@code OrderedMap}) would reduce cost to O(evictedKeys) and remove the scratch
 * allocation entirely.
 */
public final class PartitionStateEvictor {

    private PartitionStateEvictor() {
    }

    /**
     * Iterates {@code src} and copies each entry with
     * {@code lastActivityTs >= cutoffTs} into {@code dst}. The destination must
     * be empty on entry and must have been constructed with the same key/value
     * layout as {@code src}. Returns the number of entries copied.
     */
    public static long rebuildKeeping(Map src, Map dst, int lastActivityTsValueIndex, long cutoffTs) {
        MapRecordCursor cursor = src.getCursor();
        MapRecord record = src.getRecord();
        long kept = 0;
        while (cursor.hasNext()) {
            MapValue srcValue = record.getValue();
            long lastTs = srcValue.getLong(lastActivityTsValueIndex);
            if (lastTs < cutoffTs) {
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
