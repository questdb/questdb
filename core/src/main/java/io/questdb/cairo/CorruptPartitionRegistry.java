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

package io.questdb.cairo;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Partitions the checksum scrub has found to be corrupt, so a query touching one fails loudly instead
 * of returning wrong rows.
 * <p>
 * <b>Deliberately in memory only.</b> A verdict is re-derived by the scrub after a restart rather than
 * persisted, because persisting one makes a FALSE positive permanent -- and a false positive here
 * takes a healthy partition offline. Losing verdicts on restart costs at most the time until the
 * scrub reaches that partition again.
 * <p>
 * <b>Scoped to the partition, not the table.</b> Condemning a partition must not take out the rest of
 * the table: queries that touch only healthy partitions keep working. That is the whole difference
 * between this and suspending the table.
 * <p>
 * The common case is an empty registry, and it is consulted on every partition open, so the lookup
 * short-circuits on a single volatile read before touching the map.
 */
public class CorruptPartitionRegistry {
    private static final Log LOG = LogFactory.getLog(CorruptPartitionRegistry.class);

    private final ConcurrentHashMap<CharSequence, ConcurrentHashMap<String, String>> byTableDir =
            new ConcurrentHashMap<>();
    private volatile boolean empty = true;

    /**
     * Records that a partition of {@code tableToken} failed verification.
     * <p>
     * Keyed by partition DIRECTORY name rather than timestamp: that is what both the scrub and the
     * reader already hold, it needs no parsing, and it distinguishes partition versions -- an O3
     * rewrite produces a new directory, which is a different, not-yet-condemned partition.
     */
    public void condemn(TableToken tableToken, CharSequence partitionDirName, CharSequence detail) {
        final String d = detail == null ? "checksum mismatch" : detail.toString();
        byTableDir.computeIfAbsent(tableToken.getDirName().toString(), k -> new ConcurrentHashMap<>())
                .put(partitionDirName.toString(), d);
        empty = false;
        LOG.critical().$("partition condemned by checksum scrub [table=").$(tableToken)
                .$(", partition=").$(partitionDirName)
                .$(", detail=").$(d)
                .I$();
    }

    /**
     * Forgets every verdict.
     */
    public void clear() {
        byTableDir.clear();
        empty = true;
    }

    /**
     * Forgets the verdict for one partition. A verdict must be revocable: without this a false
     * positive would need a restart to clear.
     */
    public void clear(TableToken tableToken, CharSequence partitionDirName) {
        final Map<String, String> t = byTableDir.get(tableToken.getDirName());
        if (t != null) {
            t.remove(partitionDirName.toString());
        }
        recomputeEmpty();
    }

    public boolean isEmpty() {
        return empty;
    }

    /**
     * Why this partition is condemned, or null when it is not.
     */
    public String reasonFor(TableToken tableToken, CharSequence partitionDirName) {
        if (empty) {
            return null;
        }
        final Map<String, String> t = byTableDir.get(tableToken.getDirName());
        return t == null ? null : t.get(partitionDirName.toString());
    }

    public int size() {
        int n = 0;
        for (Map<String, String> t : byTableDir.values()) {
            n += t.size();
        }
        return n;
    }

    private void recomputeEmpty() {
        for (Map<String, String> t : byTableDir.values()) {
            if (!t.isEmpty()) {
                empty = false;
                return;
            }
        }
        empty = true;
    }
}
